import { Component, inject, OnInit } from '@angular/core';
import { UserService } from './Service/user.service';
import { FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { BehaviorSubject, combineLatest, Subject, take } from 'rxjs';

@Component({
  selector: 'app-root',
  imports: [ReactiveFormsModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent implements OnInit {
  title = 'reactive-app';

  userService = inject(UserService);
  passwordMatch!: boolean;

  txtInput: FormControl = new FormControl();
  behavior = new BehaviorSubject('B0');

  userForm: FormGroup = new FormGroup({
    // password: new FormControl('123', [Validators.required]),
    password: new FormControl(),
    cnfPassword: new FormControl()
  })

  password$ = this.userForm.controls['password'].valueChanges;
  confirm$ = this.userForm.controls['cnfPassword'].valueChanges;

  ngOnInit() {
    // const bSubject = new BehaviorSubject('Initial State'); // Holds 'Initial State'
    const bSubject = new Subject();
    // Subscriber A joins
    bSubject.subscribe(val => console.log('A:', val)); // Instantly logs: "Initial State"
    bSubject.next('Updated State');
    bSubject.next('More Updated State');
    // this.userService.getUsers().subscribe(res => console.log(res));
    // this.userService.forkJoinEmpty().subscribe(res => console.log(res))
    // this.userService.forkJoinNormal().subscribe(([a, b]) => console.log(a, b))
    // this.userService.forkJoinNormal().subscribe((res) => console.log(res))
    // this.userService.forkJoinNormal().subscribe((res) => console.log(res.user, res.users))

    // this.userService.getFromCombineLatest().subscribe((res) => console.log(res))
    // this.userService.combineLatestWithBehaviour().subscribe((res) => console.log(res))
    // this.userService.combineLatestWithBehaviourSwitchmap().subscribe((res) => console.log(res))

    // this.userService.combineLatestWithExample().subscribe((res) => console.log(res))


    // this.userService.mergeMapforUser().subscribe(res => console.log(res))

    // 1. Form validation depending on multiple fields Use case of combineLatest
    // 2. Combine route params + API data (Angular pattern)
    combineLatest([
      this.password$,
      this.confirm$
    ]).subscribe(([password, confirm]) => {
      console.log(password)
      console.log(confirm)
      this.passwordMatch = password === confirm;
      // console.log(this.passwordMatch)
    });

  }

  getUser() {
    // debugger;
    this.userService.user$.pipe(take(1)).subscribe(res => console.log(res))
    // this.userService.user$.subscribe(res => console.log(res))
  }

  refreshUser() {
    // this should be used in serice file
    // debugger;
    this.userService.refresh$.next();
  }



  /* 
  const subject = new Subject();
  const behavior = new BehaviorSubject('B0');
  
  combineLatest([subject, behavior]).subscribe(console.log);
  
  behavior.next('B1');
  subject.next('S1');
  subject.next('S2');
  behavior.next('B2'); 
  
  Steps of execution
  obs1 -> undefined
  obs2 -> B0

  obs1 -> undefined
  obs2 -> B1

  obs1 -> S1
  obs2 -> B1
  Emittion  [S1, B1]

  obs1 -> S2
  obs2 -> B1
  Emittion  [S2, B1]

  obs1 -> S2
  obs2 -> B2
  Emittion  [S2, B2]
  
  Final Output

  [S1, B1]
  [S2, B1]
  [S2, B2]


  ['S1', 'B1']
['S2', 'B1']
['S2', 'B2']
  
  
  */



}

